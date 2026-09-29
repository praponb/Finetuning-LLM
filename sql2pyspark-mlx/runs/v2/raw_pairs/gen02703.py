from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") != 12925)
    .select("id", "region", "sqft", F.initcap(F.trim("region")).alias("region_title"))
    .orderBy(F.col("sqft").desc(), F.col("id").desc())
)
