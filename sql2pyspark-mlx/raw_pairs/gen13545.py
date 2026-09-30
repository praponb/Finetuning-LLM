from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first(), "id")

filtered = (
    df_suppliers
    .filter(F.col("country") == "DE")
    .select(
        "*",
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
        F.when(F.col("city").isin(["Berlin", "London"]), "Tier 1").otherwise("Other").alias("city_group"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
