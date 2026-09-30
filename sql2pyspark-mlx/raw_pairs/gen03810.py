from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id")

result = (
    df_order_items
    .filter(F.col("quantity") > 4)
    .select(
        "id",
        "order_id",
        F.lead("unit_price").over(w_grp).alias("next_unit_price"),
        F.lag("quantity").over(w_grp).alias("prev_quantity"),
        F.lead("quantity").over(w_grp).alias("next_quantity"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.corr("quantity", "unit_price").over(w2).alias("corr_quantity_unit_price"),
    )
)
