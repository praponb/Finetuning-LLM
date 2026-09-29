from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").asc_nulls_last(), "id")

deduped = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select("id", "supplier_name", "country", "city", "rating", "rn")
    .orderBy(F.col("rn").desc(), "country")
)
