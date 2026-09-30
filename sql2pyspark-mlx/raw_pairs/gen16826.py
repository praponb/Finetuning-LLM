from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Bangkok", "Singapore"]))
    .select(
        "id",
        "supplier_name",
        "city",
        F.sum("rating").over(w).alias("cumulative_rating"),
        F.sum("rating").over(w2).alias("cumulative_rating_2"),
    )
)
