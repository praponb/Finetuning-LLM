from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last(), "id")

ranked = (
    df_stores
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "opened_date")
)

result = ranked.select("id", "opened_date")
