from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_products
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "supplier_id",
        F.last("supplier_id").over(w).alias("last_supplier_id"),
    )
)
