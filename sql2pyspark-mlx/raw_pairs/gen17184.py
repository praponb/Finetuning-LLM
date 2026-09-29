from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first(), "id")

base = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%ION%"))
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.trim("city").alias("clean_city"),
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
)

result = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
