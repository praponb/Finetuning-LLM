from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product_group", "holder_id").orderBy(F.col("end_date").desc(), F.col("id").desc())
w_holder_id_by_end_date_ties = Window.partitionBy("holder_id").orderBy(F.col("end_date").asc_nulls_last())
w_status_holder_id = Window.partitionBy("status", "holder_id")

base_policys = (
    df_policies
    .filter((F.col("premium").between(1042.5, 4906.3)) | (F.col("start_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.year(F.col("end_date")).alias("end_date_year"),
    )
)

annotated = (
    base_policys
    .filter(F.col("status") == "LAPSED")
    .select(
        "*",
        F.when(F.col("product").isin("AUTO", "TRAVEL"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
        (F.col("premium") + F.col("end_date_year")).alias("premium_plus_end_date_year"),
        F.date_trunc("quarter", F.col("end_date")).alias("end_date_quarter"),
    )
)

joined_holder = (
    annotated.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.end_date_year",
        "s.product_group",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

latest = (
    joined_holder
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_windows = (
    latest
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "end_date_year",
        "product_group",
        F.round(F.cume_dist().over(w_holder_id_by_end_date_ties), 3).alias("end_date_cume_dist"),
        F.round(F.col("end_date_year") - F.avg("end_date_year").over(w_status_holder_id), 2).alias("end_date_year_vs_status_holder_id_avg"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "holder_id",
        "end_date",
        "end_date_year",
        "product_group",
        "end_date_cume_dist",
        "end_date_year_vs_status_holder_id_avg",
        F.concat(F.col("product_group"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_group_ref"),
    )
)
