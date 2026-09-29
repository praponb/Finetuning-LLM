from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.length("store_name") < 9)
    .withColumn("opened_date_plus_months", F.add_months("opened_date", 6))
)

agg = (
    base
    .groupBy(
        F.when(F.col("region").isin(["North", "South"]), "core").otherwise("other").alias("region_group"),
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
    )
)

result = (
    agg
    .select("region_group", "avg_sqft", "bangkok_count")
    .orderBy("avg_sqft")
)
