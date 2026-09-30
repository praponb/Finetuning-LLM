from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", F.explode("discounts").alias("tier_value"))

result = flattened.select("id", "tier_value")
