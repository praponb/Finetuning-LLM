from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("bulk_lines").desc(), "id")
w2 = Window.orderBy("bulk_lines", "id")

unpacked = (
    df_carts
    .select(
        "id",
        "ship_to",
        F.col("attributes").getItem("gift").alias("gift"),
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
    )
)

result = (
    unpacked
    .filter(F.col("bulk_lines").between(1, 3))
    .select(
        "id",
        "gift",
        F.row_number().over(w).alias("row_seq"),
        F.lag("bulk_lines", 2).over(w2).alias("prev_bulk_lines"),
        F.lag("bulk_lines", 1, 0).over(w2).alias("prev_bulk_lines_2"),
    )
)
