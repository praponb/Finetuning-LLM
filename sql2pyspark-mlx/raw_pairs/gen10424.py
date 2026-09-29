from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.last("cost").over(w).alias("last_cost"),
    )
    .orderBy("last_cost", F.col("cost").desc())
)
