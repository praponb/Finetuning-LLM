from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id", "product_id")

result = (
    df_store_sales
    .filter(F.col("units") == 29)
    .select(
        "id",
        "sale_date",
        "store_id",
        "product_id",
        "units",
        F.row_number().over(w).alias("sale_date_row"),
        F.round((F.col("units") / F.sum("units").over(w2)), 3).alias("units_share"),
    )
)
