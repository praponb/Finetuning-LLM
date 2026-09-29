from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_suppliers
    .filter(F.length("supplier_name") > 9)
    .select(
        "id",
        "country",
        "rating",
        "city",
        "supplier_name",
        F.last("city").over(w).alias("last_city"),
        F.last("supplier_name").over(w2).alias("last_supplier_name"),
    )
)
