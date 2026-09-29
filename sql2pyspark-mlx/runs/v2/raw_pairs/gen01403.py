from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id")

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "revenue",
        "store_id",
        "units",
        F.lag("revenue", 2).over(w).alias("prev_revenue"),
        F.round((F.col("units") / F.sum("units").over(w2)), 3).alias("units_share"),
    )
    .orderBy(F.col("units_share").desc())
)
