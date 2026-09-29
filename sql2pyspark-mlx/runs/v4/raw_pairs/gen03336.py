from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("*", F.concat("country", F.lit(", "), "city").alias("country_city"))
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
