from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy(F.col("launch_date").desc_nulls_first())

result = (
    df_products
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
    )
)
