from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy("cost", "id")
w2 = Window.partitionBy(F.year("launch_date"), "brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.ntile(3).over(w).alias("cost_bucket"),
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
    )
)
