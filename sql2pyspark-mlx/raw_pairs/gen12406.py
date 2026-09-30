from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode_outer("attributes").alias("attr_key", "attr_value"))

result = (
    lines
    .select("id")
    .orderBy(F.col("id").desc())
)
