from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id")

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2024-01-30")
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "store_id",
        F.sum("units").over(w).alias("running_units"),
        F.round((F.col("units") / F.sum("units").over(w2)), 3).alias("units_share"),
    )
)
