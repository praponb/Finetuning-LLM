from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").asc_nulls_last(), "id")

ranked = (
    df_products
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
    )
    .orderBy(F.col("category").desc())
)
