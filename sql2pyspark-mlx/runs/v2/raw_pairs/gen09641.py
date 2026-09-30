from pyspark.sql import functions as F

filtered_stores = df_stores.filter((F.col("sqft") < 11189) | (F.col("city") != "Berlin"))

agg = (
    filtered_stores
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
)

result = (
    agg
    .filter(F.col("pct_bangkok") > 56)
    .select("city", "pct_bangkok")
    .orderBy("city")
    .limit(3)
)
