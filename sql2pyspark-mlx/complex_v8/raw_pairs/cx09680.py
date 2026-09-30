from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_max_premium_runnin = Window.partitionBy("status").orderBy(F.col("max_premium").desc(), "status").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_total_status_len_r = Window.partitionBy("status").orderBy(F.col("total_status_len").desc(), "status").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_total_status_len = Window.partitionBy("status").orderBy(F.col("total_status_len").desc(), "status")
w_status_by_max_premium_full = Window.partitionBy("status").orderBy(F.col("max_premium").desc(), "status").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_policys = (
    df_policies
    .filter((~F.col("status").isin("ACTIVE", "CANCELLED")) & (F.col("product") == "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.concat_ws("|", F.col("status"), F.col("product")).alias("status_product_label"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

per_group = (
    scoped_policys
    .groupBy("status")
    .agg(
        F.max("premium").alias("max_premium"),
        F.sum("status_len").alias("total_status_len"),
        F.round(100 * F.count(F.when(F.col("status") != "EXPIRED", 1)) / F.count("*"), 1).alias("pct_status_expired"),
    )
    .filter(F.col("max_premium") > 2111.3)
)

windowed = (
    per_group
    .select(
        "status",
        "max_premium",
        "total_status_len",
        "pct_status_expired",
        F.first("pct_status_expired", ignorenulls=True).over(w_status_by_max_premium_runnin).alias("first_known_pct_status_expired"),
        F.first("max_premium", ignorenulls=True).over(w_status_by_total_status_len_r).alias("first_known_max_premium"),
        (F.col("total_status_len") - F.lag("total_status_len").over(w_status_by_total_status_len)).alias("total_status_len_change"),
        F.min("pct_status_expired").over(w_status_by_total_status_len_r).alias("min_pct_status_expired_so_far"),
        F.nth_value("pct_status_expired", 2).over(w_status_by_max_premium_full).alias("second_pct_status_expired"),
    )
)

annotated = (
    windowed
    .filter(F.col("first_known_pct_status_expired") >= 10.0)
    .withColumn("status_len", F.length(F.trim(F.col("status"))))
)

screened = (
    annotated
    .crossJoin(annotated.agg(F.max("first_known_pct_status_expired").alias("overall_max_first_known_pct_status_expired")))
    .filter(F.col("first_known_pct_status_expired") < F.col("overall_max_first_known_pct_status_expired"))
    .drop("overall_max_first_known_pct_status_expired")
)

without_matches = (
    screened
    .crossJoin(screened.agg(F.avg("total_status_len").alias("overall_avg_total_status_len")))
    .filter(F.col("total_status_len") > F.col("overall_avg_total_status_len"))
    .drop("overall_avg_total_status_len")
)

result = (
    without_matches
    .select(
        "status",
        "max_premium",
        "total_status_len",
        "pct_status_expired",
        "min_pct_status_expired_so_far",
        "second_pct_status_expired",
        "status_len",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
    .orderBy(F.col("status_len").desc(), "status_prefix", "status")
    .limit(5)
)
