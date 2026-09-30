from pyspark.sql import functions as F

base = (
    df_stores
    .filter(~(F.col("store_name").like("Sia%") & ((F.col("opened_date") >= "2023-08-01") | F.col("sqft").between(8893, 10831))))
)

agg = (
    base
    .groupBy("city")
    .agg(
        F.count("*").alias("num_stores"),
        F.avg("sqft").alias("avg_sqft"),
    )
)

result = (
    agg
    .filter(F.col("num_stores") > 7)
    .select("city", "num_stores", "avg_sqft")
    .orderBy(F.col("num_stores").desc())
)
