from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.partitionBy(F.date_trunc("week", "sale_date"))
w4 = Window.partitionBy("store_id").orderBy("units")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.sum("units").over(w).alias("running_units"),
        F.min("revenue").over(w2).alias("product_id_min_revenue"),
        F.max("units").over(w3).alias("sale_date_period_max_units"),
        F.rank().over(w4).alias("units_rank"),
    )
)
