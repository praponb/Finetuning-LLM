from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2024-04-02")
    .select(
        "id",
        "status",
        "shipped_date",
        "cost",
        F.first("cost").over(w).alias("first_cost"),
    )
)
