from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")

result = (
    df_suppliers
    .select("id", "country", "city", F.count("*").over(w).alias("country_count"))
    .orderBy(F.col("id").desc(), "city")
)
