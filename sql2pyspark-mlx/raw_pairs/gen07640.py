from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_first())

base = (
    df_suppliers
    .filter(F.col("supplier_name").like("Lio%"))
    .withColumn("clean_city", F.trim("city"))
)

result = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "supplier_name", "country", "clean_city")
)
