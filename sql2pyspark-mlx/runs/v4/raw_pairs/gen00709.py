from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("country", "city").orderBy(F.col("supplier_name_length").asc_nulls_last())
w3 = Window.partitionBy("country", "city").orderBy("rating", "id")

base = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("supplier_name_length", F.length("supplier_name"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("N/A")))
)

result = (
    base
    .select(
        "*",
        F.lag("rating", 1).over(w).alias("prev_rating"),
        F.percent_rank().over(w2).alias("supplier_name_length_percent_rank"),
        F.ntile(4).over(w3).alias("rating_bucket"),
    )
)
