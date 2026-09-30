from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_dedup = Window.partitionBy("holder_id", "product").orderBy(F.col("end_date").desc(), F.col("id").desc())

prepared = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
    )
)

enriched = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.start_date_month_start",
        "s.product_clean",
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.round(F.percent_rank().over(w_holder_id_by_end_date), 3).alias("end_date_percent_rank"),
        F.dense_rank().over(w_holder_id_by_end_date).alias("end_date_dense_rank"),
    )
)

latest = (
    with_trends
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details = (
    latest.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.holder_name").alias("holder_holder_name"),
    )
)

result = (
    with_details
    .filter(F.upper(F.trim(F.col("holder_holder_name"))).like("D%"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        "start_date_month_start",
        "product_clean",
        "holder_name",
        "holder_segment",
        "holder_holder_name",
        F.round(F.col("end_date_percent_rank") * 1.5, 2).alias("end_date_percent_rank_adj"),
    )
    .orderBy("end_date")
)
