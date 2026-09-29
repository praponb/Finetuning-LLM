from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy(F.col("rating").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_suppliers
    .filter(F.col("country").isin(["US", "JP", "UK"]))
    .select(
        "id",
        "city",
        "rating",
        "supplier_name",
        F.sum("rating").over(w).alias("city_sum_rating"),
        F.last("supplier_name").over(w2).alias("last_supplier_name"),
    )
)
