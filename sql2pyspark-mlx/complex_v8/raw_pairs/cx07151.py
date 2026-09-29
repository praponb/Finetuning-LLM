from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("region", "segment").orderBy(F.col("risk_score").desc(), F.col("id").desc())

filtered_holders = (
    df_policyholders
    .filter(F.col("risk_score") < 0.7)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.concat(F.col("segment"), F.lit("#"), F.col("id").cast("string")).alias("segment_ref"),
    )
)

latest_per_key = (
    filtered_holders
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .groupBy("joined_on")
    .pivot("region", ["South", "East", "North", "West"])
    .agg(F.avg("risk_score"))
    .orderBy("joined_on")
)
