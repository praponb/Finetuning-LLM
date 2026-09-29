from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-08-27", "2025-04-01"))
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
)
