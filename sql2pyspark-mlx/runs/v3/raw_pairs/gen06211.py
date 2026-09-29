from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
    .orderBy(F.col("status").desc(), F.col("id").desc())
)
