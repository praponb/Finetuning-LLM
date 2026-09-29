from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("region", "segment").orderBy("risk_score", "id")

scoped_holders = (
    df_policyholders
    .filter((F.col("joined_on").between(F.lit("2024-03-22").cast("date"), F.lit("2025-05-01").cast("date"))) & (F.col("segment") == "RETAIL"))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
    )
)

unique_rows = (
    scoped_holders
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

tagged = (
    unique_rows
    .select(
        "*",
        F.when(F.col("region").isin("West", "East"), "group_a").when(F.col("region") == "North", "group_b").otherwise("other").alias("region_group"),
        F.length(F.trim(F.col("segment"))).alias("segment_len"),
        F.length(F.trim(F.col("region"))).alias("region_len"),
    )
)

result = (
    tagged
    .select("id", "segment_len", "region_len")
    .unpivot(
        ids=["id"],
        values=["segment_len", "region_len"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
