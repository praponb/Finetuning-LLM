from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("basket_amount").desc(), "id")
w2 = Window.orderBy(F.col("basket_amount").desc())

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "ship_to",
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("basket_amount").between(500, 800))
    .select(
        "id",
        "basket_amount",
        F.row_number().over(w_part).alias("seq_no"),
        F.rank().over(w2).alias("basket_amount_rank"),
    )
)
