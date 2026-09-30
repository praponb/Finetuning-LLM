from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("is_singapore").desc_nulls_first())

cleaned = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .withColumn("city_upper", F.upper("city"))
    .withColumn("is_singapore", F.when(F.col("city") == "Singapore", 1).otherwise(0))
)

result = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)
