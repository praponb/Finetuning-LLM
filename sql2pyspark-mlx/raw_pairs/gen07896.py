from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("adjusted_quantity", "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-07-31"))) & (F.col("product_id") == 7))
    .select("*", F.round((F.col("quantity") * 0.85), 2).alias("adjusted_quantity"))
)

result = (
    filtered
    .withColumn("quantity_bucket", F.ntile(5).over(w_part))
    .withColumn("row_seq", F.row_number().over(w2))
)
