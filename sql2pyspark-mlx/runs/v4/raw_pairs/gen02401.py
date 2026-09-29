from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").asc_nulls_last(), "id")

ranked = (
    df_sessions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select("id", "customer_id", "browser", "country")
    .orderBy(F.col("country").desc_nulls_last(), "id")
)
