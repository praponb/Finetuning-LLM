from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("delivered_date")).orderBy(F.col("cost").desc(), "id")
w2 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "cost",
        F.ntile(10).over(w).alias("cost_bucket"),
        F.first("cost", ignorenulls=True).over(w2).alias("first_known_cost"),
    )
)
