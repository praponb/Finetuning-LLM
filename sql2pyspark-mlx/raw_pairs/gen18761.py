from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")

deduped = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.bit_and("units").alias("units_bit_and"),
        F.count("*").alias("event_count"),
    )
    .orderBy("cnt")
)
