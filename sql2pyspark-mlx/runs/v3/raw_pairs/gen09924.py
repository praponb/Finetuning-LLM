from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "supplier_name",
        F.lag("rating", 2).over(w).alias("prev_rating"),
        F.last("supplier_name").over(w2).alias("last_supplier_name"),
    )
)
