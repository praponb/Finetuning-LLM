from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .select(
        "id",
        "status",
        "shipped_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
    .orderBy(F.col("running_cost").desc())
)
