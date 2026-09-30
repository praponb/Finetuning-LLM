from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating_rounded")

filtered = (
    df_suppliers
    .filter(F.lower(F.trim("supplier_name")) == "eurogoods gmbh")
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.round("rating", 1).alias("rating_rounded"),
        F.upper("supplier_name").alias("supplier_name_upper"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)
