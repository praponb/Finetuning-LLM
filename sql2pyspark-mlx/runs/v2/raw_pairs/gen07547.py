from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        "product_id",
        "sale_date",
        F.first("revenue").over(w).alias("first_revenue"),
        F.sum("units").over(w2).alias("running_units"),
    )
    .orderBy(F.col("sale_date").desc())
)
