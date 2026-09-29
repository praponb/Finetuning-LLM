from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Pacifi%"))
    .select("id", "city", "rating", F.avg("rating").over(w).alias("city_avg_rating"))
    .orderBy("city")
)
