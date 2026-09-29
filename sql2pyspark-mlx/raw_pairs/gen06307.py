from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("stock_qty").asc_nulls_first())
w2 = Window.partitionBy("brand")
w_grp = Window.orderBy("stock_qty", "id")
w4 = Window.partitionBy(F.date_trunc("WEEK", "launch_date"), "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("category")

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "supplier_id",
        F.rank().over(w).alias("stock_qty_rank"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.first("brand", ignorenulls=True).over(w4).alias("first_known_brand"),
        F.corr("price", "cost").over(w_part).alias("corr_price_cost"),
    )
)
