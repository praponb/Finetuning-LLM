from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id").orderBy(F.col("last_restock_minus_79d").desc(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id")
w4 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w_recent = Window.partitionBy("product_id").orderBy("last_restock_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-06-29")))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_sub("last_restock", 79).alias("last_restock_minus_79d"),
        F.dayofweek("last_restock").alias("last_restock_dayofweek"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.row_number().over(w_hist).alias("seq_no"),
        F.max("last_restock_dayofweek").over(w2).alias("product_id_max_last_restock_dayofweek"),
        F.ntile(4).over(w3).alias("quantity_bucket"),
        F.lag("quantity", 1).over(w4).alias("prev_quantity"),
        F.first("quantity", ignorenulls=True).over(w_recent).alias("first_known_quantity"),
    )
)
