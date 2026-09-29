from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "device").orderBy(F.col("started_at_dayofweek").desc(), "id")

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & F.col("browser").isin(["Firefox", "Chrome"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.dayofweek("started_at").alias("started_at_dayofweek"),
        F.coalesce("country", F.lit("OTHER")).alias("country_clean"),
    )
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "customer_id", "device", "browser", "started_at_dayofweek")
)
