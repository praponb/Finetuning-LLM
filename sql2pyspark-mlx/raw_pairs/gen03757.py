from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("k", "attr_value"))
)

result = (
    lines
    .select("id", "created_at", "k", "attr_value")
    .orderBy(F.col("k").asc_nulls_first())
)
