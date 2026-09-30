from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("carrier")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "cost",
        "carrier",
        F.last("carrier").over(w).alias("last_carrier"),
        F.sum("cost").over(w2).alias("carrier_sum_cost"),
    )
)
