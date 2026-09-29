from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.sum("units").over(w).alias("running_units"),
        F.last("revenue").over(w2).alias("last_revenue"),
    )
)
