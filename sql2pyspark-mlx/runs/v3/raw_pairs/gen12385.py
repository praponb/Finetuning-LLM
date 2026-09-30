from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        "product_id",
        "sale_date",
        F.round((F.col("units") / F.sum("units").over(w)), 3).alias("units_share"),
        F.lag("units", 2).over(w2).alias("prev_units"),
    )
)
