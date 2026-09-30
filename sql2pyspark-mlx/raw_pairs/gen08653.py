from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter((F.lower(F.trim("supplier_name")) == "tokyo traders") & (F.col("rating") > 2.1))
)

agg = (
    filtered_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("rating")).otherwise(0)).alias("new_york_rating"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)

result = (
    agg
    .select("country", "new_york_rating")
    .orderBy("country", "new_york_rating")
)
