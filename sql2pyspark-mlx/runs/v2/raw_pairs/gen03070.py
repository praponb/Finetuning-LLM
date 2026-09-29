from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-05-14")
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
    .orderBy("running_cost")
)
