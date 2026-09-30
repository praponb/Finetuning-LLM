from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc())

ranked = (
    df_suppliers
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "supplier_name", "country")
)

result = (
    ranked
    .groupBy("country")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("num_rows").desc())
)
