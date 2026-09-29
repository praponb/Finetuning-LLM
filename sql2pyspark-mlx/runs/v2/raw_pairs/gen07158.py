from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_products
    .select(
        "id",
        "cost",
        "supplier_id",
        F.last("supplier_id").over(w).alias("last_supplier_id"),
    )
    .orderBy("last_supplier_id")
)
