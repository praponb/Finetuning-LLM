from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_last())

deduped = (
    df_suppliers
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .select("id", "supplier_name", "country", "city", "rating")
    .orderBy(F.col("rating").desc_nulls_first())
)
