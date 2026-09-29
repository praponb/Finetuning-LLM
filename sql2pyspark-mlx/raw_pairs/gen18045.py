from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").asc_nulls_last())
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("rating").asc_nulls_first())

filtered = (
    df_suppliers
    .filter(F.col("rating") < 1.8)
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.concat_ws("-", "supplier_name", "city").alias("supplier_name_city_key"),
    )
)

result = (
    filtered
    .filter(F.col("country") == "SG")
    .withColumn("rating_dense_rank", F.dense_rank().over(w))
    .withColumn("last_known_rating", F.last("rating", ignorenulls=True).over(w2))
    .withColumn("rating_rank", F.rank().over(w3))
)
