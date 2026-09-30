from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_by_start_d = Window.partitionBy("holder_id", "product").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date = Window.orderBy("end_date", "id")
w_all_by_end_date_ties = Window.orderBy("end_date")
w_latest = Window.partitionBy("product", "holder_id").orderBy("end_date_quintile", "id")

base = (
    df_policies
    .filter(F.col("status").isin("CANCELLED", "EXPIRED", "ACTIVE"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
)

with_trends = (
    base
    .select(
        "*",
        F.first("start_date").over(w_holder_id_product_by_start_d).alias("first_start_date"),
        F.ntile(5).over(w_all_by_end_date).alias("end_date_quintile"),
        F.round(F.percent_rank().over(w_all_by_end_date_ties), 3).alias("end_date_percent_rank"),
        F.row_number().over(w_all_by_end_date).alias("rn"),
    )
)

latest = (
    with_trends
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details_holder = (
    latest.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.first_start_date",
        "s.end_date_quintile",
        "s.end_date_percent_rank",
        "s.rn",
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    with_details_holder
    .rollup("product", "rn")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.max("first_start_date").alias("last_first_start_date"),
        F.round(100 * F.count(F.when(F.lower(F.col("holder_name")).like("%en%"), 1)) / F.count("*"), 1).alias("pct_lower_holder_name_like_en"),
    )
)
