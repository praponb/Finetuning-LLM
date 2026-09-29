from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_suppliers
    .select(
        "id",
        "city",
        F.sum("rating").over(w).alias("running_rating"),
        F.last("city", ignorenulls=True).over(w_hist).alias("last_known_city"),
    )
)

result = (
    with_windows
    .select("id", "city", "running_rating", "last_known_city")
    .orderBy(F.col("city").desc())
)
