from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("distinct_categories").desc())

result = (
    df_products
    .groupBy("brand", F.date_trunc("QUARTER", "launch_date").alias("quarter_start"))
    .agg(
        F.countDistinct("category").alias("distinct_categories"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("cost")).otherwise(0)).alias("initech_cost"),
    )
    .withColumn("distinct_categories_percent_rank", F.percent_rank().over(w))
)
