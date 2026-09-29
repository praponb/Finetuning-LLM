from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("store_id") == 2)
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "units",
        F.sum("revenue").over(w).alias("running_revenue"),
        F.first("units").over(w2).alias("first_units"),
    )
)
