from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date")
w2 = Window.partitionBy("product_id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "product_id",
        "units",
        F.rank().over(w).alias("sale_date_rank"),
        F.round((F.col("units") / F.sum("units").over(w2)), 3).alias("units_share"),
    )
)
