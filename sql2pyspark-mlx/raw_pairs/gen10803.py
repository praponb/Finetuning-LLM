from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("browser").orderBy(F.col("country_length").desc())

base = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-05-03"))) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.length("country").alias("country_length"),
        F.substring("browser", 1, 4).alias("browser_prefix"),
    )
)

result = (
    base
    .filter(F.to_date("started_at") < "2025-06-16")
    .select(
        "id",
        "customer_id",
        F.max("country_length").over(w).alias("country_length_max_90d"),
        F.rank().over(w2).alias("country_length_rank"),
    )
)
