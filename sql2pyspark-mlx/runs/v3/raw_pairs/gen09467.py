from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("city", "country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.0)
    .select(
        "id",
        "city",
        "rating",
        "supplier_name",
        "country",
        F.last("supplier_name").over(w).alias("last_supplier_name"),
        F.row_number().over(w2).alias("row_num"),
    )
)
