from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock_year")

result = (
    df_inventory
    .filter(F.col("quantity") == 152)
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_quantity", F.sum("total_quantity").over(w))
)
